from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-08-04").cast("date"))
    .withColumn("opened_date_year", F.year("opened_date"))
)

metrics = (
    filtered
    .select(
        "id",
        "opened_date",
        F.row_number().over(w).alias("row_seq"),
        F.avg("opened_date_year").over(w2).alias("opened_date_year_avg_5"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.year(F.col("c.sale_date")) >= 2024), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"), F.col("c.sale_date"))
)

valid = (
    with_latest
    .filter(F.col("opened_date_year_avg_5") != 48)
    .withColumn("months_sale_date_to_opened_date", F.round((F.datediff("opened_date", "sale_date") / 30), 1))
)

result = (
    valid
    .select("id", "opened_date", "row_seq", "opened_date_year_avg_5", "revenue")
    .orderBy(F.col("id").asc_nulls_first(), "opened_date_year_avg_5")
)
