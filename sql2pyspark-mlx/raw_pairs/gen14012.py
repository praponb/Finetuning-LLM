from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "region")
    .exceptAll(
        df_stores
        .filter(F.col("sqft") >= 5681)
        .select("id", "region")
    )
)
