from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "Tokyo"]))
    .select(
        "id",
        "city",
        "sqft",
        "opened_date",
        "region",
        F.row_number().over(w).alias("row_num"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
    .orderBy("running_sqft")
)
