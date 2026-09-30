from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .subtract(
        df_customers
        .filter(F.col("age") < 31)
        .select(F.col("signup_date").alias("delivered_date"))
    )
    .orderBy(F.col("delivered_date").desc())
)
