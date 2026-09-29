from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(10165, 12717))
    .select("id", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("region") == "East")
        .select("id", "opened_date")
    )
    .distinct()
)
