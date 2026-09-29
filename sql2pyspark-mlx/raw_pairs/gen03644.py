from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 15521)
    .select("city", "region")
    .unionByName(
        df_stores
        .filter(F.col("sqft").between(11275, 12542))
        .select("city", "region")
    )
)
