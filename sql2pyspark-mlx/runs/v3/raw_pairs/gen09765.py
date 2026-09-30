from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 9190)
    .select("id", "sqft")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["New York", "Berlin"]))
        .select("id", "sqft")
    )
    .distinct()
)
