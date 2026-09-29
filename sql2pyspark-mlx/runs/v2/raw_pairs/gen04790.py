from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") != "/cart")
    .select("page")
    .unionByName(
        df_web_events
        .filter(F.col("page").isin(["/checkout", "/home", "/cart"]))
        .select("page")
    )
)
