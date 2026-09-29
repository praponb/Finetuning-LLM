from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("product_id").orderBy("units", "id")
w6 = Window.orderBy("revenue_avg_2_prior", "id")
w7 = Window.partitionBy("store_id", "product_id").orderBy("corr_revenue_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("row_seq", "id")
w9 = Window.orderBy("units", "id")
w10 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("product_id").orderBy(F.col("units").asc_nulls_last())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

enriched = (
    df_store_sales
    .select(
        "*",
        F.corr("revenue", "units").over(w).alias("corr_revenue_units"),
        F.avg("revenue").over(w2).alias("revenue_avg_2_prior"),
        F.sum("units").over(w3).alias("cumulative_units"),
        F.corr("revenue", "units").over(w4).alias("corr_revenue_units_2"),
        F.ntile(5).over(w5).alias("units_bucket"),
    )
)

scored = (
    enriched
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("first_known_corr_revenue_units", F.first("corr_revenue_units", ignorenulls=True).over(w7))
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

scored_store_sales = (
    ranked
    .withColumn("units_bucket_2", F.ntile(10).over(w9))
    .withColumn("cumulative_corr_revenue_units", F.sum("corr_revenue_units").over(w10))
    .withColumn("units_cume_dist", F.cume_dist().over(w11))
)

result = (
    scored_store_sales
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "corr_revenue_units",
        "cumulative_units",
        "units_bucket",
        "row_seq",
        F.col("sum_inventory_quantity"),
    )
)
