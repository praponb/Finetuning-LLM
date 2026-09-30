from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id")
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("units").alias("sum_store_sale_units"))

latest = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.store_name"))
)

base = (
    joined
    .filter(F.col("last_restock") > F.lit("2023-09-30").cast("date"))
    .withColumn("store_name_title", F.initcap(F.trim("store_name")))
    .withColumn("dedup_rank_str", F.col("dedup_rank").cast("double"))
)

result = (
    base
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "dedup_rank",
        "store_name",
        "store_name_title",
        F.when(F.col("store_name").rlike("^(Mall Central|Siam Flagship)$"), "PRIORITY").otherwise("CORE").alias("store_name_zone"),
        F.col("sum_store_sale_units"),
    )
    .orderBy(F.col("store_id").asc_nulls_last())
)
