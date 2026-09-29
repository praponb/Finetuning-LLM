from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id", "product_id").orderBy("units", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("sale_date_dayofweek_avg_6_prior").asc_nulls_first(), "id")

valid = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6, 8, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.dayofweek("sale_date").alias("sale_date_dayofweek"),
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.avg("units").over(w_grp).alias("last7_avg_units"),
        F.max("units").over(w_grp).alias("last7_max_units"),
        F.count("units").over(w_grp).alias("last7_count_units"),
    )
)

enriched_store_sales = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "revenue_per_unit",
        F.avg("sale_date_dayofweek").over(w_ordered).alias("sale_date_dayofweek_avg_6_prior"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

ranked = (
    enriched_store_sales
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank_2", F.row_number().over(w4))
    .filter(F.col("latest_rank_2") == 1)
)

result = (
    latest
    .select(
        "id",
        "product_id",
        "revenue",
        "revenue_per_unit",
        "seq_no",
        "latest_rank",
        "latest_rank_2",
    )
    .orderBy(F.col("revenue_per_unit").desc())
)
