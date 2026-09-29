from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id_tier").orderBy(F.col("distinct_customer_ids").desc())

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("product_id") >= 11, "LARGE").otherwise("SMALL").alias("product_id_tier"),
        "promo",
    )
    .agg(
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count("*").alias("row_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .withColumn("distinct_customer_ids_rank", F.rank().over(w))
    .orderBy(F.col("distinct_customer_ids").asc_nulls_first())
)
