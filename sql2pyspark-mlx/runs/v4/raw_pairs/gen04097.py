from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft") >= 6346)
        .select("id", "city")
    )
)
