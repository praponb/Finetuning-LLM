from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select("segment", "age")
    .unionByName(
        df_customers
        .filter(F.month("signup_date").isin([8, 10, 12]))
        .select("segment", "age")
    )
    .distinct()
)
