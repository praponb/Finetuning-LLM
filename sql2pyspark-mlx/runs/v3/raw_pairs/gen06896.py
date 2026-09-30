from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .groupBy(
        F.when(F.col("segment").isin(["Corporate", "Consumer"]), "priority").otherwise("other").alias("segment_group"),
        "is_active",
    )
    .agg(F.sum("age").alias("total_age"))
    .orderBy(F.col("total_age").desc())
)
