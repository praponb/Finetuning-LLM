from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy(F.date_trunc("month", "opened_date"), "city").orderBy(F.col("sqft").desc_nulls_last())
w4 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.row_number().over(w2).alias("seq_no"),
        F.max("sqft").over(w).alias("region_max_sqft"),
        F.dense_rank().over(w3).alias("sqft_dense_rank"),
        F.sum("sqft").over(w4).alias("cumulative_sqft"),
    )
)

result = (
    scored
    .select("id", "opened_date", "sqft", "sqft_share", "seq_no", "sqft_dense_rank")
    .orderBy(F.col("id").desc_nulls_first())
)
