from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy(F.date_trunc("month", "opened_date"), "city")

base = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-06"))) & F.col("region").isNotNull())
    .withColumn("store_name_title", F.initcap(F.trim("store_name")))
    .withColumn("sqft_num", F.col("sqft").cast("double"))
)

result = (
    base
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "store_name_title",
        "sqft_num",
        F.lead("sqft_num").over(w).alias("next_sqft_num"),
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("seq_no"),
        F.min("sqft").over(w2).alias("opened_date_period_min_sqft"),
    )
)
