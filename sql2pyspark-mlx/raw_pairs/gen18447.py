from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("opened_date"), "city")

result = (
    df_stores
    .filter(F.col("sqft").between(6664, 9394))
    .select(
        "*",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
    )
)
