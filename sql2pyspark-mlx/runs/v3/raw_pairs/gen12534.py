from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select("email", "name")
    .unionByName(
        df_customers
        .filter(F.col("segment") == "Corporate")
        .select("email", "name")
    )
)
