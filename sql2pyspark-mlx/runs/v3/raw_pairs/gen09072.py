from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 10040)
    .select("opened_date")
    .unionByName(
        df_customers
        .filter(F.col("country") == "JP")
        .select(F.col("signup_date").alias("opened_date"))
    )
    .distinct()
    .orderBy(F.col("opened_date").desc())
)
