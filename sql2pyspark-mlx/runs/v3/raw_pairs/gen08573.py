from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") == 2025)
    .select("store_name", "region")
    .intersect(
        df_stores
        .filter(~F.col("store_name").like("%Mall%"))
        .select("store_name", "region")
    )
)
