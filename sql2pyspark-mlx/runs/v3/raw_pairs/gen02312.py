from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") > 58) & F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
)
