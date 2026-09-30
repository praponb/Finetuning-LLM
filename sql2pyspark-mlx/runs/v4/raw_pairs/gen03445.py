from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("books_count").desc())

result = (
    df_products
    .groupBy(
        F.date_trunc("quarter", "launch_date").alias("quarter_start"),
        F.date_trunc("month", "launch_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
    )
    .withColumn("books_count_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("activity_month").desc())
)
