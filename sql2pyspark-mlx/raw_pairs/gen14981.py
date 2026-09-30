from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element")
w2 = Window.orderBy(F.col("element_max_product_id_share").asc_nulls_first())
w3 = Window.partitionBy("element").orderBy(F.col("element_avg_element_sum_product_id_str").asc_nulls_first())

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "coupon_codes", F.col("line.product_id"))
)

flattened = exploded.select("product_id", F.explode_outer("coupon_codes").alias("element"))

valid = (
    flattened
    .filter(F.col("element").isNotNull() & (F.col("product_id") >= 9))
    .select(
        "*",
        F.col("product_id").cast("double").alias("product_id_num"),
        F.col("product_id").cast("double").alias("product_id_str"),
    )
)

enriched = (
    valid
    .select(
        "product_id",
        "element",
        F.sum("product_id_str").over(w).alias("element_sum_product_id_str"),
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
    )
)

scored = (
    enriched
    .filter(F.col("element").isin(["SPRING10", "VIP", "BUNDLE"]))
    .withColumn("element_max_product_id_share", F.max("product_id_share").over(w))
    .withColumn("product_id_share_2", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
    .withColumn("element_avg_element_sum_product_id_str", F.avg("element_sum_product_id_str").over(w))
)

result = (
    scored
    .select(
        "element_sum_product_id_str",
        "product_id_share_2",
        "element_avg_element_sum_product_id_str",
        F.rank().over(w2).alias("element_max_product_id_share_rank"),
        F.dense_rank().over(w3).alias("element_avg_element_sum_product_id_str_dense_rank"),
        F.max("element_max_product_id_share").over(w).alias("element_max_element_max_product_id_share"),
    )
)
