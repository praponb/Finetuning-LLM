from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "region")

result = (
    df_stores
    .select(
        "*",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
    )
)
