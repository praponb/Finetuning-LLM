from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select("id", "name")
    .unionByName(
        df_customers
        .filter(F.col("city").isin(["London", "Bangkok"]))
        .select("id", "name")
    )
    .distinct()
)
