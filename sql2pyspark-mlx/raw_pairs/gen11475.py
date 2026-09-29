from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc_nulls_last(), "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("next_revenue").desc_nulls_first(), "id")

filtered = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 10]))
    .select("*", F.date_trunc("year", "sale_date").alias("sale_date_period"))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .filter(F.col("units").between(26, 32))
    .select(
        "*",
        F.lag("units").over(w2).alias("prev_units"),
        F.lag("revenue").over(w2).alias("prev_revenue"),
        F.lead("revenue").over(w2).alias("next_revenue"),
        F.row_number().over(w2).alias("seq_no"),
        F.first("units", ignorenulls=True).over(w3).alias("first_known_units"),
    )
)

ranked_store_sales = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    ranked_store_sales
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .select("id", "store_id", "revenue", "prev_revenue", "next_revenue")
    .unpivot(
        ids=["id", "store_id"],
        values=["revenue", "prev_revenue", "next_revenue"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
)
