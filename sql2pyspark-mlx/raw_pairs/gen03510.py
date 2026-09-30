from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([1, 7, 8]))
    .select("id", "sqft", "region")
    .subtract(
        df_stores
        .filter(~F.col("store_name").like("%Flagship%"))
        .select("id", "sqft", "region")
    )
)
