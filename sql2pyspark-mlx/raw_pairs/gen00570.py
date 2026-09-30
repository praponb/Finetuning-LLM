from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price", "id")
w2 = Window.partitionBy("store_id")
w3 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("units", "id")
w5 = Window.partitionBy(F.year("sale_date"), "store_id").orderBy(F.col("price").desc(), "id")
w6 = Window.orderBy(F.col("units_share").desc(), "id")
w7 = Window.partitionBy(F.year("sale_date"), "store_id")
w8 = Window.partitionBy(F.month("sale_date"), "store_id").orderBy("units_share", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w10 = Window.partitionBy("store_id").orderBy("next_stock_qty")

joined = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.price"), F.col("p.stock_qty"))
)

enriched = (
    joined
    .filter(F.col("store_id") == 6)
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.lead("stock_qty", 1).over(w).alias("next_stock_qty"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.sum("revenue").over(w3).alias("cumulative_revenue"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.row_number().over(w6).alias("seq_no"),
        F.stddev_samp("event_seq").over(w7).alias("sale_date_year_stddev_samp_event_seq"),
        F.avg("event_seq").over(w8).alias("event_seq_avg_6_prior"),
        F.dense_rank().over(w9).alias("revenue_dense_rank"),
    )
)

joined_store_sales = (
    scored
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.sale_date",
        "a.revenue",
        "a.next_stock_qty",
        "a.units_share",
        "a.row_seq",
        "a.event_seq",
        "a.revenue_dense_rank",
        F.col("p.store_name"),
    )
)

enriched_store_sales = (
    joined_store_sales
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.opened_date"))
)

deduped = (
    enriched_store_sales
    .withColumn("latest_rank", F.rank().over(w10))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select("id", "store_id", "revenue", "units_share")
    .unpivot(
        ids=["id", "store_id"],
        values=["revenue", "units_share"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
