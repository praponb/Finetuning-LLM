from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 9300)
    .select("name")
    .unionByName(
        df_customers
        .filter(F.col("segment").isin(["Corporate", "SMB"]))
        .select("name")
    )
    .distinct()
)
