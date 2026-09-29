from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("region") == "South")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.ceil("sqft").alias("sqft_whole"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", "s.sqft", F.col("c.sale_date"), F.col("c.units"))
)
