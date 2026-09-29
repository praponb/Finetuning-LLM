from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("delivered_date"), "carrier")

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "carrier",
        "cost",
        F.sum("cost").over(w).alias("delivered_date_year_sum_cost"),
    )
)
