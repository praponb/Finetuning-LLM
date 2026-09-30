from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .select("id", "age", "country")
    .unionByName(
        df_customers
        .filter(F.col("city") == "Berlin")
        .select("id", "age", "country")
    )
    .orderBy("country")
)
