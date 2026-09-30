from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "order_id")
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy(F.col("latest_rank").desc(), "id")
w6 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id")
w8 = Window.partitionBy("order_id").orderBy(F.col("first_known_cost").asc_nulls_first())

cleaned = (
    df_order_items
    .filter(F.col("unit_price") < 120)
    .select("*", (F.col("unit_price") + F.col("quantity")).alias("line_total"))
)

joined = (
    cleaned
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.brand"), F.col("p.cost"))
)

latest = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .withColumn("product_id_min_unit_price", F.min("unit_price").over(w2))
    .withColumn("brand_sum_quantity", F.sum("quantity").over(w3))
    .withColumn("unit_price_avg_2", F.avg("unit_price").over(w4))
)

scored = (
    metrics
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w6))
)

cleaned_order_items = (
    scored
    .filter(F.col("brand").isNotNull() & (F.col("brand") == "Initech"))
    .withColumn("product_id_min_unit_price_per_first_known_cost", F.round((F.col("product_id_min_unit_price") / F.col("first_known_cost")), 2))
    .withColumn("brand_group", F.when(F.col("brand").isin(["Stark", "Initech"]), "Priority").otherwise("Other"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("OTHER")))
)

with_windows = (
    cleaned_order_items
    .select(
        "*",
        F.count("cost").over(w7).alias("product_id_count_cost"),
        F.dense_rank().over(w8).alias("first_known_cost_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "quantity",
        "unit_price",
        "line_total",
        "brand",
        "cost",
        "latest_rank",
        "product_id_min_unit_price",
        "brand_sum_quantity",
        F.when(F.col("brand").rlike("^(Stark|Initech)$"), "CORE").otherwise("GROUP_B").alias("brand_zone"),
    )
)
