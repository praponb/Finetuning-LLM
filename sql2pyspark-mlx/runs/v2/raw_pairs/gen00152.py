from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Bangkok"]))
    .select("id", "segment")
    .intersect(
        df_customers
        .filter(F.col("city") == "Chiang Mai")
        .select("id", "segment")
    )
    .orderBy("id")
)
