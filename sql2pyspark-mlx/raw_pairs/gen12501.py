from pyspark.sql import functions as F

result = (
    df_stores
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("sqft") <= 11569)
        .select("region", "id")
    )
)
