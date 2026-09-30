from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
    )
)

metrics = flattened.withColumn("customer_id_sum_product_id", F.sum("product_id").over(w))

with_dims = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.name").like("%Pim%"))
    .select("e.*", F.col("b.country"))
)

cleaned = (
    with_dims
    .filter(F.col("country").isNotNull())
    .select(
        "*",
        (F.col("qty") - F.col("customer_id_sum_product_id")).alias("qty_minus_customer_id_sum_product_id"),
    )
)

result = (
    cleaned
    .withColumn("product_id_tier", F.when(F.col("product_id") >= 9, "HIGH").otherwise("LOW"))
    .rollup("product_id_tier")
    .agg(
        F.percentile_approx("qty", 0.9).alias("qty_p90"),
        F.percentile_approx("customer_id_sum_product_id", 0.5).alias("customer_id_sum_product_id_p50"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("qty_p90")
)
