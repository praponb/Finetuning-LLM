from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("region", "city")
w3 = Window.partitionBy("city").orderBy("sqft", "id")
w4 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.month("opened_date")).orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        F.lead("sqft", 1, 0).over(w).alias("next_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.lead("sqft", 1).over(w3).alias("next_sqft_2"),
        F.sum("sqft").over(w4).alias("running_sqft"),
        F.avg("sqft").over(w5).alias("sqft_avg_6"),
    )
)
