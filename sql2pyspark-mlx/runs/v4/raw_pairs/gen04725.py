from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("customer_id", "code").orderBy(F.col("age").asc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy(F.col("age").desc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

joined = (
    exploded
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.age"))
)

enriched = (
    joined
    .withColumn("age_sum_1h", F.sum("age").over(w))
    .withColumn("age_dense_rank", F.dense_rank().over(w2))
    .withColumn("age_percent_rank", F.percent_rank().over(w3))
)

result = (
    enriched
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("created_at").alias("first_created_at"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.sum("age_percent_rank").alias("total_age_percent_rank"),
    )
    .filter(F.col("total_age_percent_rank") >= 0)
    .orderBy(F.col("customer_id").asc_nulls_first())
)
