from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") <= 26), "left_anti")
)

result = not_flagged.select("id", "city", "region", "opened_date")
