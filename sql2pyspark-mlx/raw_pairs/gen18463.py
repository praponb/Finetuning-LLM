from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("units").desc())
w5 = Window.partitionBy("product_id", "brand").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id")

latest = (
    df_store_sales
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

scored = (
    deduped
    .filter(F.col("rank_in_group_2") >= 1)
    .select(
        "id",
        "product_id",
        "revenue",
        "rank_in_group",
        "rank_in_group_2",
        F.first("units", ignorenulls=True).over(w3).alias("first_known_units"),
        F.ntile(4).over(w).alias("units_bucket"),
        F.percent_rank().over(w4).alias("units_percent_rank"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.brand"), F.col("d.price"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "product_id",
        "rank_in_group",
        "rank_in_group_2",
        "first_known_units",
        "units_percent_rank",
        "price",
        F.first("first_known_units", ignorenulls=True).over(w5).alias("first_known_first_known_units"),
        F.round((F.col("first_known_units") / F.sum("first_known_units").over(w6)), 4).alias("first_known_units_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_id",
        F.col("units_percent_rank").alias("UNITS_PERCENT_RANK"),
        F.col("first_known_units_share").alias("FIRST_KNOWN_UNITS_SHARE"),
        "price",
    )
    .unpivot(
        ids=["id", "product_id"],
        values=["UNITS_PERCENT_RANK", "FIRST_KNOWN_UNITS_SHARE", "price"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
