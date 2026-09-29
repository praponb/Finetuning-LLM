from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "age", "country")
    .unionByName(
        df_customers
        .filter(F.col("city") != "Tokyo")
        .select("id", "age", "country")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
