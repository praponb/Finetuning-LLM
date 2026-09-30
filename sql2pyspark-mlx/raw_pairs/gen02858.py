from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
