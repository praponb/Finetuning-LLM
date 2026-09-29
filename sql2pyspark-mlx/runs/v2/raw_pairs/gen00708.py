from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2023-08-01")
    .select("id", "opened_date", "store_name")
    .intersect(
        df_stores
        .filter(~F.col("store_name").like("M%"))
        .select("id", "opened_date", "store_name")
    )
    .orderBy(F.col("store_name").desc())
)
