from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "is_active",
        "city",
        F.when(F.col("segment").isin(["Corporate", "SMB"]), "group_a").otherwise("other").alias("segment_group"),
    )
    .agg(F.max("credit_limit").alias("max_credit_limit"))
    .filter(F.col("max_credit_limit") >= 5600)
    .orderBy(F.col("max_credit_limit").desc())
)
