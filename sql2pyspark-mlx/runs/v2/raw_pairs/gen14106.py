from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") >= 35) & F.col("credit_limit").between(6800, 11200) & F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
    .select(
        "name",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
        F.initcap(F.trim("segment")).alias("segment_title"),
    )
    .orderBy(F.col("credit_limit_int").desc())
)
