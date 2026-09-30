from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("region", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("Mal%"))
        .select("region", "opened_date")
    )
    .distinct()
)
