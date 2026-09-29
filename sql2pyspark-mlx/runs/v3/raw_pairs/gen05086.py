from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Singapore")
    .select("store_name")
    .intersect(
        df_stores
        .filter(F.month("opened_date").isin([1, 5, 9]))
        .select("store_name")
    )
)
