from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(~(F.col("s.sqft").between(7338, 9191) & (F.col("s.opened_date") <= "2023-10-01")))
    .select(F.col("ss.units"), F.col("s.sqft"), F.col("s.city"))
    .orderBy("units")
)
