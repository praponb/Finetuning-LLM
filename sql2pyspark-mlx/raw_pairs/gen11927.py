from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("segment", "name")
    .unionByName(
        df_customers
        .filter(F.col("city").isin(["Bangkok", "Berlin"]))
        .select("segment", "name")
    )
    .distinct()
)
