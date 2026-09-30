from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_first())
w3 = Window.partitionBy(F.year("sale_date"), "store_id")
w_ordered = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("units").alias("sum_store_sale_units"))

enriched = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 7, 9]))
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
        F.corr("units", "revenue").over(w3).alias("corr_units_revenue"),
        F.sum("units").over(w_ordered).alias("running_units"),
    )
)

result = (
    enriched
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "seq_no",
        "units_dense_rank",
        "running_units",
        F.col("sum_store_sale_units"),
    )
    .orderBy("seq_no")
)
