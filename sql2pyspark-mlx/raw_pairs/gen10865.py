from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "opened_date"), "city")

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "city",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
    )
    .orderBy(F.col("id").desc())
)
