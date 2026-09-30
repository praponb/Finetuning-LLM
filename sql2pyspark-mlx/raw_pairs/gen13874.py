from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("supplier_id", "brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy(F.year("launch_date"), "category").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand", "category")

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-09-05").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("tags_count", F.size("tags"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("UNKNOWN")))
)

with_windows = (
    valid
    .select(
        "*",
        F.lead("cost").over(w_grp).alias("next_cost"),
        F.lead("stock_qty").over(w_grp).alias("next_stock_qty"),
        F.lag("stock_qty").over(w_grp).alias("prev_stock_qty"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "resolved_brand",
        "next_cost",
        "row_seq",
        F.first("prev_stock_qty", ignorenulls=True).over(w2).alias("first_known_prev_stock_qty"),
        F.sum("row_seq").over(w3).alias("running_row_seq"),
        F.max("row_seq").over(w4).alias("brand_max_row_seq"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(~F.col("b.country").isin(["JP", "TH", "US"]))
    .select("e.*", F.col("b.city"))
)

result = (
    joined
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["country", "supplier_city"], ["country"], []],
        F.col("s.country").alias("country"),
        F.col("s.city").alias("supplier_city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.stock_qty")), 2).alias("avg_stock_qty"),
        F.max(F.col("a.row_seq")).alias("max_row_seq"),
    )
)
