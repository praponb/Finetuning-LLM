from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("opened_date"), "city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy(F.month("opened_date"), "city").orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "region").orderBy(F.col("sqft").desc_nulls_first())
w_ordered = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, -1)

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-07").cast("date")) & F.col("region").isNotNull())
    .withColumn("sqft_str", F.col("sqft").cast("string"))
)

result = (
    cleaned
    .select(
        "id",
        "store_name",
        F.ntile(5).over(w).alias("sqft_bucket"),
        F.avg("sqft").over(w2).alias("sqft_avg_5"),
        F.dense_rank().over(w3).alias("sqft_dense_rank"),
        F.avg("sqft").over(w_ordered).alias("sqft_avg_2_prior"),
    )
)
