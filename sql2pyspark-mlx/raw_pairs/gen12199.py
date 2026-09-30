from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_first())
w2 = Window.orderBy(F.col("units").asc_nulls_first())
w3 = Window.orderBy("revenue")
w4 = Window.orderBy("units")
w5 = Window.partitionBy("store_id").orderBy(F.col("units_rank").desc())
w6 = Window.partitionBy("store_id")
w7 = Window.orderBy(F.col("units_dense_rank_2").desc_nulls_last())
w8 = Window.partitionBy(F.year("sale_date"))
w9 = Window.partitionBy("store_id", "product_id")

kept_keys = (
    df_store_sales
    .select("store_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("sale_date").between("2024-02-10", "2024-04-01"))
        .select("store_id", "id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("revenue") <= 3600)
    .select(
        "store_id",
        "product_id",
        "sale_date",
        F.dense_rank().over(w).alias("units_dense_rank"),
        F.dense_rank().over(w2).alias("units_dense_rank_2"),
        F.dense_rank().over(w3).alias("revenue_dense_rank"),
        F.rank().over(w4).alias("units_rank"),
    )
)

scored = (
    with_windows
    .select(
        "product_id",
        "sale_date",
        F.rank().over(w5).alias("units_rank_rank"),
        F.round((F.col("revenue_dense_rank") / F.sum("revenue_dense_rank").over(w6)), 4).alias("revenue_dense_rank_share"),
        F.dense_rank().over(w7).alias("units_dense_rank_2_dense_rank"),
        F.round((F.col("revenue_dense_rank") / F.sum("revenue_dense_rank").over(w8)), 4).alias("revenue_dense_rank_share_2"),
        F.round((F.col("units_rank") / F.sum("units_rank").over(w9)), 4).alias("units_rank_share"),
    )
)

filtered = (
    scored
    .filter(F.col("units_rank_rank") < 6)
    .select(
        "*",
        F.round((F.col("revenue_dense_rank_share") * 0.85), 2).alias("revenue_dense_rank_share_adjusted"),
    )
)

result = (
    filtered
    .select(
        "product_id",
        F.col("units_rank_share").alias("UNITS_RANK_SHARE"),
        "revenue_dense_rank_share",
        F.col("revenue_dense_rank_share_2").alias("REVENUE_DENSE_RANK_SHARE_2_VAL"),
        F.col("revenue_dense_rank_share_adjusted").alias("REVENUE_DENSE_RANK_SHARE_ADJUSTED"),
    )
    .unpivot(
        ids=["product_id"],
        values=["UNITS_RANK_SHARE", "revenue_dense_rank_share", "REVENUE_DENSE_RANK_SHARE_2_VAL", "REVENUE_DENSE_RANK_SHARE_ADJUSTED"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
