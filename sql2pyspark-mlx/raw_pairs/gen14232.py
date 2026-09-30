from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("quantity").desc_nulls_last())
w3 = Window.partitionBy("city").orderBy(F.col("quantity").desc(), "id")
w_hist = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.month("opened_date"), "city").orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-05-11").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

metrics = (
    with_last
    .filter(F.col("opened_date_month_end") > "2025-02-01")
    .withColumn("quantity_dense_rank", F.dense_rank().over(w2))
    .withColumn("quantity_bucket", F.ntile(3).over(w3))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w_hist))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w_grp))
)

eligible = (
    metrics
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date")), "left_anti")
)

eligible_stores = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([2, 5, 7])), "left_anti")
)

result = (
    eligible_stores
    .select(
        "id",
        "city",
        "opened_date",
        "opened_date_month_end",
        "quantity",
        "quantity_dense_rank",
        "quantity_bucket",
        "last_known_quantity",
        "first_known_quantity",
        F.when(F.col("city").rlike("^(London|Singapore)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("CORE").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
