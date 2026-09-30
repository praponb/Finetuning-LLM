from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity_whole").desc(), "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w_grp = Window.orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("unit_price") < 240)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.when(F.col("unit_price") >= 430, "High").when(F.col("unit_price") >= 200, "Medium").otherwise("Low").alias("unit_price_band"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.lead("unit_price", 1).over(w2).alias("next_unit_price"),
        F.avg("quantity").over(w_grp).alias("quantity_avg_5"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

cleaned = (
    scored
    .filter(F.col("row_seq").between(5, 6))
    .select(
        "id",
        "product_id",
        "unit_price",
        "unit_price_band",
        "quantity_whole",
        "event_seq",
        "next_unit_price",
        "quantity_avg_5",
        "row_seq",
        F.floor("event_seq").alias("event_seq_whole"),
        F.when(F.col("event_seq") >= 7, "High").when(F.col("event_seq") >= 2, "Medium").otherwise("Low").alias("event_seq_band"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.brand") != "Umbrella")
    .select("a.*", F.col("p.category"), F.col("p.price"), F.col("p.cost"))
)

joined = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.category").alias("product_category"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
)

result = (
    joined
    .select(
        "id",
        "unit_price",
        "unit_price_band",
        "event_seq",
        "next_unit_price",
        "quantity_avg_5",
        "row_seq",
        "event_seq_band",
        "category",
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
