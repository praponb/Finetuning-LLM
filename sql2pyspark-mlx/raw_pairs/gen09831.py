from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("opened_date").between("2025-03-01", "2025-10-01"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.revenue").alias("store_sale_revenue"))
)
