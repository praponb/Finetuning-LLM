from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("segment").isin(["Corporate", "Consumer"]), "core").otherwise("other").alias("segment_group"),
    )
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .orderBy("segment_group")
    .limit(5)
)
