from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNull())
    .select("id", "city")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit") <= 11400)
        .select("id", "city")
    )
    .orderBy("id")
)
