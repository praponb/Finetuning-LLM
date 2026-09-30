from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 15001)
    .select("region")
    .unionByName(
        df_stores
        .filter(F.col("opened_date").between("2024-01-30", "2024-07-30"))
        .select("region")
    )
)
