from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "store_id").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.orderBy("sale_date_month_end", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 8, 9]))
    .select("*", F.last_day("sale_date").alias("sale_date_month_end"))
)

joined = (
    base
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.store_name"), F.col("b.region"), F.col("b.opened_date"))
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "sale_date", "units", "revenue", "sale_date_month_end", "store_name")
)

with_windows = (
    deduped
    .filter(F.col("store_name").like("%Downto%"))
    .withColumn("last3_avg_revenue", F.avg("revenue").over(w2))
    .withColumn("last3_sum_revenue", F.sum("revenue").over(w2))
    .withColumn("last3_count_revenue", F.count("revenue").over(w2))
)

kept_keys = (
    with_windows
    .select("id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units") > 22)
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "revenue",
        "sale_date_month_end",
        "last3_sum_revenue",
        "last3_count_revenue",
    )
    .orderBy(F.col("sale_date_month_end").desc_nulls_last(), "last3_count_revenue")
)
