from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("spring10_rows").desc())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

result = (
    flattened
    .groupBy("element", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("first_seen"),
        F.max("created_at").alias("last_seen"),
        F.max("created_at").alias("ended_at"),
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .withColumn("spring10_rows_rank", F.rank().over(w))
)
