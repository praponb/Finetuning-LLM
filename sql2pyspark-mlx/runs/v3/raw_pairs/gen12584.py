from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") != 60) & ((F.col("segment") == "SMB") | (F.col("credit_limit") < 15000)))
)
