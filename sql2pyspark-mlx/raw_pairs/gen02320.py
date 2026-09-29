from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_stores
    .select(
        "*",
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.lead("sqft").over(w).alias("next_sqft"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "prev_sqft",
        "next_sqft",
    )
    .orderBy(F.col("city").asc_nulls_last())
)
