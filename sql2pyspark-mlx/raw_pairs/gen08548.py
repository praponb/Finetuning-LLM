from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity_str", "id")
w2 = Window.partitionBy(F.year("launch_date"), "product_id").orderBy(F.col("launch_date").desc(), "id")
w3 = Window.partitionBy("brand", "product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("brand").orderBy("row_seq", "id")

filtered = (
    df_order_items
    .filter(F.col("unit_price") < 300)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

metrics = (
    filtered
    .withColumn("next_quantity", F.lead("quantity").over(w))
    .withColumn("prev_quantity", F.lag("quantity").over(w))
    .withColumn("next_line_total", F.lead("line_total").over(w))
    .withColumn("prev_line_total", F.lag("line_total").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)

enriched = (
    metrics
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.stock_qty") > 353)
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.line_total",
        "s.quantity_str",
        "s.prev_line_total",
        "s.event_seq",
        F.col("b.brand"),
        F.col("b.launch_date"),
    )
)

enriched_order_items = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.launch_date").alias("product_launch_date"))
)

scored = (
    enriched_order_items
    .select(
        "id",
        "quantity",
        "event_seq",
        "brand",
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(5).over(w3).alias("quantity_bucket"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .select(
        "id",
        "brand",
        "row_seq",
        F.when(F.col("brand").rlike("^(Globex|Initech)$"), "OTHER_ZONE").when(F.col("brand").rlike("^(Acme|Umbrella)$"), "CORE").otherwise("EXTENDED").alias("brand_zone"),
    )
    .orderBy(F.col("row_seq").asc_nulls_last())
)
