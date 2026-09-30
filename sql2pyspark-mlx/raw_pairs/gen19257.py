from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region")

result = (
    df_stores
    .filter(F.upper("store_name").like("%ALL%"))
    .select(
        "id",
        "store_name",
        "sqft",
        F.avg("sqft").over(w).alias("sqft_avg_2_prior"),
        F.sum("sqft").over(w2).alias("running_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)
