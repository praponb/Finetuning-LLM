from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("greatest_unit_price_quantity").desc(), "id")
w_part = Window.partitionBy("order_id", "product_id").orderBy(F.col("greatest_unit_price_quantity").desc())
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w_hist = Window.partitionBy("product_id").orderBy("unit_price", "id")
w5 = Window.orderBy(F.col("unit_price").desc(), "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("unit_price_bucket").desc_nulls_last())
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .filter(F.col("unit_price").between(120, 250))
    .select(
        "id",
        "product_id",
        "unit_price",
        "greatest_unit_price_quantity",
        "row_num",
        F.percent_rank().over(w_part).alias("greatest_unit_price_quantity_percent_rank"),
        F.dense_rank().over(w3).alias("unit_price_dense_rank"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.ntile(10).over(w5).alias("unit_price_bucket"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

daily = (
    ranked
    .groupBy("product_id")
    .agg(
        F.avg("unit_price_bucket").alias("avg_unit_price_bucket"),
        F.percentile_approx("greatest_unit_price_quantity_percent_rank", [0.25, 0.5, 0.75]).alias("greatest_unit_price_quantity_percent_rank_quartiles"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("avg_unit_price_bucket") > 0)
)

result = (
    daily
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "avg_unit_price_bucket",
        "row_count",
        F.col("min_order_item_unit_price"),
    )
)
